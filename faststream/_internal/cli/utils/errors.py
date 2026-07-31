from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typer._click.exceptions import BadParameter as BadParameterType

    from faststream.exceptions import StartupValidationError


def _get_click_exceptions() -> tuple[type[Any], type[Any]]:
    try:
        from typer._click.exceptions import BadParameter, MissingParameter
    except ImportError:
        from click.exceptions import (
            BadParameter as ClickBadParameter,
            MissingParameter as ClickMissingParameter,
        )

        return ClickBadParameter, ClickMissingParameter
    else:
        return BadParameter, MissingParameter


def draw_startup_errors(startup_exc: StartupValidationError) -> None:
    from typer.core import TyperOption

    bad_parameter, missing_parameter = _get_click_exceptions()

    def draw_error(click_exc: BadParameterType) -> None:
        try:
            from typer import rich_utils

            rich_utils.rich_format_error(click_exc)
        except ImportError:
            click_exc.show()

    for field in startup_exc.invalid_fields:
        draw_error(
            bad_parameter(
                message=(
                    "extra option in your application "
                    "`lifespan/on_startup` hook has a wrong type."
                ),
                param=TyperOption(param_decls=[f"--{field}"]),
            ),
        )

    if startup_exc.missed_fields:
        draw_error(
            missing_parameter(
                message=(
                    "You registered extra options in your application "
                    "`lifespan/on_startup` hook, but does not set in CLI."
                ),
                param=TyperOption(
                    param_decls=[f"--{x}" for x in startup_exc.missed_fields],
                ),
            ),
        )
