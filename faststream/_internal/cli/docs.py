import json
import sys
import warnings
from contextlib import suppress
from pathlib import Path
from pprint import pformat
from typing import TYPE_CHECKING, cast

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

from .dto import RunArgs
from .options import (
    APP_ARGUMENT,
    APP_DIR_OPTION,
    FACTORY_OPTION,
    RELOAD_EXTENSIONS_OPTION,
    RELOAD_FLAG,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from faststream.specification.base import SpecificationFactory


docs_app = typer.Typer(pretty_exceptions_short=True)


@docs_app.command(name="serve")
def serve(
    docs: str = typer.Argument(
        ...,
        help="[python_module:FastStream] or [asyncapi.json/.yaml] - path to your application or documentation.",
        show_default=False,
    ),
    host: str = typer.Option(
        "localhost",
        help="Documentation hosting address.",
    ),
    port: int = typer.Option(
        8000,
        help="Documentation hosting port.",
    ),
    app_dir: str = APP_DIR_OPTION,
    is_factory: bool = FACTORY_OPTION,
    reload: bool = RELOAD_FLAG,
    watch_extensions: list[str] = RELOAD_EXTENSIONS_OPTION,
) -> None:
    """Serve project AsyncAPI schema."""
    if ":" in docs:
        if app_dir:  # pragma: no branch
            sys.path.insert(0, app_dir)

        module, _ = import_from_string(docs, is_factory=is_factory)

        module_parent = module.parent
        extra_extensions: Sequence[str] = watch_extensions

    else:
        module_parent = Path.cwd()
        schema_filepath = module_parent / docs
        extra_extensions = (schema_filepath.suffix, *watch_extensions)

    run_args = RunArgs(
        app=docs,
        extra_options={"host": host, "port": port},
        is_factory=is_factory,
    )

    if reload:
        try:
            from faststream._internal.cli.supervisors.watchfiles import WatchReloader

        except ImportError:
            warnings.warn(INSTALL_WATCHFILES, category=ImportWarning, stacklevel=1)
            _parse_and_serve(run_args)

        else:
            WatchReloader(
                target=_parse_and_serve,
                args=run_args,
                reload_dirs=(str(module_parent),),
                extra_extensions=extra_extensions,
            ).run()

    else:
        _parse_and_serve(run_args)


@docs_app.command(name="gen")
def gen(
    app: str = APP_ARGUMENT,
    yaml: bool = typer.Option(
        False,
        "-y",
        "--yaml",
        is_flag=True,
        help="Generate `asyncapi.yaml` schema.",
    ),
    out: str | None = typer.Option(
        None,
        "-o",
        "--out",
        help="Output filename.",
        show_default="asyncapi.json/.yaml",
    ),
    debug: bool = typer.Option(
        False,
        "-d",
        "--debug",
        is_flag=True,
        help="Do not save generated schema to file. Print it instead.",
    ),
    app_dir: str = APP_DIR_OPTION,
    is_factory: bool = FACTORY_OPTION,
) -> None:
    """Generate project AsyncAPI schema."""
    if app_dir:  # pragma: no branch
        sys.path.insert(0, app_dir)

    _, app_obj = import_from_string(app, is_factory=is_factory)
    schema_factory = cast(
        "SpecificationFactory | None",
        getattr(app_obj, "schema", None),
    )
    if not schema_factory:
        msg = f"{app_obj} doesn't have `schema` attribute"
        raise ValueError(msg)

    raw_schema = schema_factory.to_specification()

    if yaml:
        try:
            schema = raw_schema.to_yaml()
        except ImportError as e:  # pragma: no cover
            typer.echo(INSTALL_YAML, err=True)
            raise typer.Exit(1) from e

        filename = out or "asyncapi.yaml"

        if not debug:
            Path(filename).write_text(schema, encoding="utf-8")
    else:
        schema = raw_schema.to_jsonable()
        filename = out or "asyncapi.json"

        if not debug:
            with Path(filename).open("w", encoding="utf-8") as f:
                json.dump(schema, f, indent=2)

        else:
            schema = pformat(schema)

    if debug:
        typer.echo("Generated schema:\n")
        typer.echo(schema, color=True)

    else:
        typer.echo(f"Your project AsyncAPI scheme was placed to `{filename}`")


def _parse_and_serve(args: RunArgs) -> None:
    if ":" in args.app:
        _, app_obj = import_from_string(args.app, is_factory=args.is_factory)
        schema_factory = cast(
            "SpecificationFactory | None",
            getattr(app_obj, "schema", None),
        )
        if not schema_factory:
            msg = f"{app_obj} doesn't have `schema` attribute"
            raise ValueError(msg)
        raw_schema = schema_factory.to_specification()

    else:
        schema_filepath = Path.cwd() / args.app

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
            msg = f"Unknown extension given - {args.app}; Please provide app in format [python_module:Specification] or [asyncapi.yaml/.json] - path to your application or documentation"
            raise ValueError(msg)

        for schema in (SchemaV3, SchemaV2_6):
            with suppress(ValidationError):
                raw_schema = model_parse(schema, data)
                break
        else:
            typer.echo(SCHEMA_NOT_SUPPORTED.format(schema_filename=args.app), err=True)
            raise typer.Exit(1)

    serve_app(
        raw_schema,
        cast("str", args.extra_options.get("host", "localhost")),
        cast("int", args.extra_options.get("port", 8000)),
    )
