from faststream.exceptions import StartupValidationError


def draw_startup_errors(startup_exc: StartupValidationError) -> None:
    from click.exceptions import BadParameter, MissingParameter
    from typer.core import TyperOption

    def draw_error(click_exc: BadParameter) -> None:
        try:
            from typer import rich_utils

            rich_utils.rich_format_error(click_exc)
        except ImportError:
            click_exc.show()

    for field in startup_exc.invalid_fields:
        draw_error(
            BadParameter(
                message=(
                    "extra option in your application "
                    "`lifespan/on_startup` hook has a wrong type."
                ),
                param=TyperOption(param_decls=[f"--{field}"]),
            ),
        )

    if startup_exc.missed_fields:
        draw_error(
            MissingParameter(
                message=(
                    "You registered extra options in your application "
                    "`lifespan/on_startup` hook, but does not set in CLI."
                ),
                param=TyperOption(
                    param_decls=[f"--{x}" for x in startup_exc.missed_fields],
                ),
            ),
        )
