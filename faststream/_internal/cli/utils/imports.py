import importlib
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import typer

from faststream.exceptions import SetupError


def import_from_string(
    import_str: str,
    *,
    is_factory: bool = False,
) -> tuple[Path, object]:
    module_path, instance = _import_object_or_factory(import_str)

    if is_factory:
        if callable(instance):
            instance = instance()
        else:
            msg = f'"{instance}" is not a factory.'
            raise typer.BadParameter(msg)

    return module_path, instance


def _import_object_or_factory(import_str: str) -> tuple[Path, object]:
    """Import FastStream application from module specified by a string."""
    if not isinstance(import_str, str):
        msg = "Given value is not of type string"
        raise typer.BadParameter(msg)

    module_str, _, attrs_str = import_str.partition(":")
    if not module_str or not attrs_str:
        msg = f'Import string "{import_str}" must be in format "<module>:<attribute>"'
        raise typer.BadParameter(
            msg,
        )

    try:
        module = importlib.import_module(  # nosemgrep: python.lang.security.audit.non-literal-import.non-literal-import
            module_str,
        )

    except ModuleNotFoundError:
        module_path, import_obj_name = _get_obj_path(import_str)
        instance = _try_import_app(module_path, import_obj_name)

    else:
        attr = module
        try:
            for attr_str in attrs_str.split("."):
                attr = getattr(attr, attr_str)
            instance = attr

        except AttributeError as e:
            typer.echo(e, err=True)
            msg = f'Attribute "{attrs_str}" not found in module "{module_str}".'
            raise typer.BadParameter(
                msg,
            ) from e

        if module.__file__:
            module_path = Path(module.__file__).resolve().parent
        else:
            module_path = Path.cwd()

    return module_path, instance


def _try_import_app(module: Path, app: str) -> object:
    """Tries to import a FastStream app from a module."""
    try:
        app_object = _import_object(module, app)

    except FileNotFoundError as e:
        typer.echo(e, err=True)
        msg = "Please, input module like [python_file:docs_object] or [module:attribute]"
        raise typer.BadParameter(
            msg,
        ) from e

    else:
        return app_object


def _import_object(module: Path, app: str) -> object:
    """Import an object from a module."""
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
        msg = f"{spec} has no loader"
        raise SetupError(msg)

    loader.exec_module(mod)

    try:
        obj = getattr(mod, app)
    except AttributeError as e:
        raise FileNotFoundError(module) from e

    return obj


def _get_obj_path(obj_path: str) -> tuple[Path, str]:
    """Get the application path."""
    if ":" not in obj_path:
        msg = f"`{obj_path}` is not a path to object"
        raise SetupError(msg)

    module, app_name = obj_path.split(":", 2)

    mod_path = Path.cwd()
    for i in module.split("."):
        mod_path /= i

    return mod_path, app_name
