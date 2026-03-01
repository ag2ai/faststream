"""CLI entry point to FastStream framework."""

from faststream._internal._compat import HAS_TYPER

if not HAS_TYPER:
    msg = (
        "\n\nYou're trying to use the FastStream CLI, "
        "\nbut you haven't installed the required dependencies."
        "\nPlease install them using the following command: "
        '\npip install "faststream[cli]"'
    )
    raise ImportError(msg)

import warnings

warnings.filterwarnings("default", category=ImportWarning, module="faststream")

from faststream._internal.cli.main import cli  # noqa: E402

if __name__ == "__main__":
    cli(prog_name="faststream")
