"""Pluggable YAML parsers for `faststream docs` commands.

`PyYAML` is the default; `ruamel.yaml` is offered as a built-in alternative.
Users can plug in any parser via a `module:callable` reference (see #2709).
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
    """Resolve a YAML parser by name (`auto`, `pyyaml`, `ruamel`) or `module:callable`."""
    if name is None or name == "auto":
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
