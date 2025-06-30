import typer

FACTORY_OPTION = typer.Option(
    False,
    "-f",
    "--factory",
    help="Treat APP as an application factory.",
)

RELOAD_FLAG = typer.Option(
    False,
    "-r",
    "--reload",
    is_flag=True,
    help="Restart app at directory files changes.",
)

APP_DIR_OPTION = typer.Option(
    ".",
    "--app-dir",
    help=("Look for APP in the specified directory, by adding this to the PYTHONPATH."),
    envvar="FASTSTREAM_APP_DIR",
)

RELOAD_EXTENSIONS_OPTION = typer.Option(
    (),
    "--extension",
    "--ext",
    "--reload-extension",
    "--reload-ext",
    help="List of file extensions to watch by.",
)

APP_ARGUMENT = typer.Argument(
    ...,
    help="[python_module:FastStream] - path to your application.",
    show_default=False,
)
