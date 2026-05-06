"""Pluggable YAML parsers for `faststream docs` commands.

The default `PyYAML` parser is a YAML 1.1 implementation and does not preserve
all YAML 1.2 typings that the AsyncAPI specification requires (see #2709).
This module exposes a small registry so users can opt into `ruamel.yaml`
(which is YAML 1.2 compliant) or plug in their own parser via a dotted path.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from faststream._internal.cli.utils.imports import import_from_string
from faststream.exceptions import INSTALL_YAML, SetupError

if TYPE_CHECKING:
    from pathlib import Path


YamlParser = Callable[["Path"], Any]


def _pyyaml_parser(path: Path) -> Any:
    try:
        import yaml
    except ImportError as e:  # pragma: no cover
        raise SetupError(INSTALL_YAML) from e

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _ruamel_parser(path: Path) -> Any:
    try:
        from ruamel.yaml import YAML
    except ImportError as e:
        msg = (
            "`ruamel.yaml` is not installed. Install it with "
            "`pip install ruamel.yaml` to use the ruamel parser, or pick a "
            "different parser via `--yaml-parser`."
        )
        raise SetupError(msg) from e

    yaml = YAML(typ="safe", pure=True)
    with path.open("r", encoding="utf-8") as f:
        return yaml.load(f)


_BUILTIN_PARSERS: dict[str, YamlParser] = {
    "pyyaml": _pyyaml_parser,
    "ruamel": _ruamel_parser,
}


def get_yaml_parser(name: str | None = None) -> YamlParser:
    """Resolve a YAML parser by name or import path.

    Args:
        name: One of `auto` (default), `pyyaml`, `ruamel`, or a dotted import
            path of the form `module.path:callable`. The callable must accept
            a `pathlib.Path` and return the parsed document.

    Returns:
        The selected parser callable.

    Raises:
        SetupError: when `name` is not a known built-in and is not a valid
            `module:callable` reference.
    """
    if name is None or name == "auto":
        # Prefer ruamel if it is installed: the AsyncAPI 3.0 spec recommends
        # YAML 1.2, which `PyYAML` does not fully support.
        try:
            import ruamel.yaml  # noqa: F401
        except ImportError:
            return _pyyaml_parser
        return _ruamel_parser

    if name in _BUILTIN_PARSERS:
        return _BUILTIN_PARSERS[name]

    if ":" in name:
        _, parser = import_from_string(name)
        if not callable(parser):
            msg = f"`{name}` does not resolve to a callable."
            raise SetupError(msg)
        return cast("YamlParser", parser)

    msg = (
        f"Unknown YAML parser `{name}`. Use one of "
        f"{sorted(_BUILTIN_PARSERS)}, `auto`, or a dotted "
        f"`module:callable` reference."
    )
    raise SetupError(msg)
