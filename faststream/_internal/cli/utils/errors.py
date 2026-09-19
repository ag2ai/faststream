from typer.core import TyperOption

from faststream.exceptions import StartupValidationError

try:
    from typer._click import exceptions as click_exceptions
except ImportError:  # pragma: no cover - Typer < 0.26
    from click import exceptions as click_exceptions  # type: ignore[no-redef]

try:
    from typer.rich_utils import rich_format_error
except ImportError:  # typer-slim ships without rich

    def rich_format_error(self: click_exceptions.ClickException) -> None:
        self.show()


def draw_startup_errors(startup_exc: StartupValidationError) -> None:
    for field in startup_exc.invalid_fields:
        rich_format_error(
            click_exceptions.BadParameter(
                message=(
                    "extra option in your application "
                    "`lifespan/on_startup` hook has a wrong type."
                ),
                param=TyperOption(param_decls=[f"--{field}"]),
            ),
        )

    if startup_exc.missed_fields:
        rich_format_error(
            click_exceptions.MissingParameter(
                message=(
                    "You registered extra options in your application "
                    "`lifespan/on_startup` hook, but does not set in CLI."
                ),
                param=TyperOption(
                    param_decls=[f"--{x}" for x in startup_exc.missed_fields],
                ),
            ),
        )
