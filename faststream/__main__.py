"""CLI entry point to FastStream library."""

import warnings

from faststream._internal._compat import HAS_TYPER

if not HAS_TYPER:
    raise ImportError(
        "\n\nYou're trying to use the FastStream CLI, "
        "\nbut you haven't installed the required dependencies."
        "\nPlease install them using the following command: "
        '\npip install "faststream[cli]"'
    )

from faststream._internal.cli.main import cli

warnings.filterwarnings("default", category=ImportWarning, module="faststream")

if __name__ == "__main__":
    cli(prog_name="faststream")
