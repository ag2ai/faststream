from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Tuple

import typer

from faststream.app import FastStream


def try_import_app(module: Path, app: str) -> FastStream:
    """Tries to import a FastStream app from a module.

    Args:
        module: Path to the module containing the app.
        app: Name of the FastStream app.

    Returns:
        The imported FastStream app object.

    Raises:
        FileNotFoundError: If the module file is not found.
        typer.BadParameter: If the module or app name is not provided correctly.
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    try:
        app_object = import_object(module, app)

    except FileNotFoundError as e:
        typer.echo(e, err=True)
        raise typer.BadParameter(
            "Please, input module like [python_file:faststream_app_name]"
        ) from e

    else:
        return app_object  # type: ignore


def import_object(module: Path, app: str) -> object:
    """Import an object from a module.

    Args:
        module: The path to the module file.
        app: The name of the object to import.

    Returns:
        The imported object.

    Raises:
        FileNotFoundError: If the module file is not found.
        ValueError: If the module has no loader.
        AttributeError: If the object is not found in the module.
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    spec = spec_from_file_location(
        "mode",
        f"{module}.py",
        submodule_search_locations=[str(module.parent.absolute())],
    )

    if spec is None:  # pragma: no cover
        raise FileNotFoundError(module)

    mod = module_from_spec(spec)
    loader = spec.loader

    if loader is None:  # pragma: no cover
        raise ValueError(f"{spec} has no loader")

    loader.exec_module(mod)

    try:
        obj = getattr(mod, app)
    except AttributeError as e:
        raise FileNotFoundError(module) from e

    return obj


def get_app_path(app: str) -> Tuple[Path, str]:
    """Get the application path.

    Args:
        app (str): The name of the application in the format "module:app_name".

    Returns:
        Tuple[Path, str]: A tuple containing the path to the module and the name of the application.

    Raises:
        ValueError: If the given app is not in the format "module:app_name".
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    if ":" not in app:
        raise ValueError(f"{app} is not a FastStream")

    module, app_name = app.split(":", 2)

    mod_path = Path.cwd()
    for i in module.split("."):
        mod_path = mod_path / i

    return mod_path, app_name
