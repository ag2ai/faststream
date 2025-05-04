import json
import sys
import warnings
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import typer
from pydantic import ValidationError

from faststream._internal._compat import json_dumps, model_parse
from faststream._internal.cli.utils.imports import import_from_string
from faststream.exceptions import INSTALL_WATCHFILES, INSTALL_YAML, SCHEMA_NOT_SUPPORTED
from faststream.specification.asyncapi.site import serve_app
from faststream.specification.asyncapi.v2_6_0.schema import (
    ApplicationSchema as SchemaV2_6,
)
from faststream.specification.asyncapi.v3_0_0.schema import (
    ApplicationSchema as SchemaV3,
)
from faststream.specification.base.specification import Specification

if TYPE_CHECKING:
    from collections.abc import Sequence

docs_app = typer.Typer(pretty_exceptions_short=True)


@docs_app.command(name="serve")
def serve(
    docs: str = typer.Argument(
        ...,
        help="[python_module:Specification] or [asyncapi.yaml/.json] - path to your application or documentation.",
    ),
    host: str = typer.Option(
        "localhost",
        help="Documentation hosting address.",
    ),
    port: int = typer.Option(
        8000,
        help="Documentation hosting port.",
    ),
    reload: bool = typer.Option(
        False,
        "--reload",
        is_flag=True,
        help="Restart documentation at directory files changes.",
    ),
    app_dir: str = typer.Option(
        ".",
        "--app-dir",
        help=(
            "Look for APP in the specified directory, by adding this to the PYTHONPATH."
            " Defaults to the current working directory."
        ),
    ),
    is_factory: bool = typer.Option(
        False,
        "--factory",
        is_flag=True,
        help="Treat APP as an application factory.",
    ),
) -> None:
    """Serve project AsyncAPI schema."""
    if ":" in docs:
        if app_dir:  # pragma: no branch
            sys.path.insert(0, app_dir)

        module, _ = import_from_string(docs, is_factory=is_factory)

        module_parent = module.parent
        extra_extensions: Sequence[str] = ()

    else:
        module_parent = Path.cwd()
        schema_filepath = module_parent / docs
        extra_extensions = (schema_filepath.suffix,)

    if reload:
        try:
            from faststream._internal.cli.supervisors.watchfiles import WatchReloader

        except ImportError:
            warnings.warn(INSTALL_WATCHFILES, category=ImportWarning, stacklevel=1)
            _parse_and_serve(docs, host, port, is_factory)

        else:
            WatchReloader(
                target=_parse_and_serve,
                args=(docs, host, port, is_factory),
                reload_dirs=(str(module_parent),),
                extra_extensions=extra_extensions,
            ).run()

    else:
        _parse_and_serve(docs, host, port, is_factory)


@docs_app.command(name="gen")
def gen(
    asyncapi: str = typer.Argument(
        ...,
        help="[python_module:Specification] - path to your AsyncAPI object.",
    ),
    yaml: bool = typer.Option(
        False,
        "--yaml",
        is_flag=True,
        help="Generate `asyncapi.yaml` schema.",
    ),
    out: Optional[str] = typer.Option(
        None,
        help="Output filename.",
    ),
    app_dir: str = typer.Option(
        ".",
        "--app-dir",
        help=(
            "Look for APP in the specified directory, by adding this to the PYTHONPATH."
            " Defaults to the current working directory."
        ),
    ),
    is_factory: bool = typer.Option(
        False,
        "--factory",
        is_flag=True,
        help="Treat APP as an application factory.",
    ),
    asyncapi_version: str = typer.Option(
        "3.0.0",
        "--version",
        help="Version of asyncapi schema. Currently supported only 3.0.0 and 2.6.0",
    ),
) -> None:
    """Generate project AsyncAPI schema."""
    if app_dir:  # pragma: no branch
        sys.path.insert(0, app_dir)

    _, asyncapi_obj = import_from_string(asyncapi, is_factory=is_factory)

    assert isinstance(asyncapi_obj, Specification)  # nosec B101

    raw_schema = asyncapi_obj.schema

    if yaml:
        try:
            schema = raw_schema.to_yaml()
        except ImportError as e:  # pragma: no cover
            typer.echo(INSTALL_YAML, err=True)
            raise typer.Exit(1) from e

        name = out or "asyncapi.yaml"

        with Path(name).open("w", encoding="utf-8") as f:
            f.write(schema)

    else:
        schema = raw_schema.to_jsonable()
        name = out or "asyncapi.json"

        with Path(name).open("w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2)

    typer.echo(f"Your project AsyncAPI scheme was placed to `{name}`")


def _parse_and_serve(
    docs: str,
    host: str = "localhost",
    port: int = 8000,
    is_factory: bool = False,
) -> None:
    if ":" in docs:
        _, docs_obj = import_from_string(docs, is_factory=is_factory)

        assert isinstance(docs_obj, Specification)  # nosec B101

        raw_schema = docs_obj

    else:
        schema_filepath = Path.cwd() / docs

        if schema_filepath.suffix == ".json":
            data = schema_filepath.read_bytes()

        elif schema_filepath.suffix in {".yaml", ".yml"}:
            try:
                import yaml
            except ImportError as e:  # pragma: no cover
                typer.echo(INSTALL_YAML, err=True)
                raise typer.Exit(1) from e

            with schema_filepath.open("r") as f:
                schema = yaml.safe_load(f)

            data = json_dumps(schema)

        else:
            msg = f"Unknown extension given - {docs}; Please provide app in format [python_module:Specification] or [asyncapi.yaml/.json] - path to your application or documentation"
            raise ValueError(
                msg,
            )

        for schema in (SchemaV3, SchemaV2_6):
            with suppress(ValidationError):
                raw_schema = model_parse(schema, data)
                break
        else:
            typer.echo(SCHEMA_NOT_SUPPORTED.format(schema_filename=docs), err=True)
            raise typer.Exit(1)

    serve_app(raw_schema, host, port)
