"""Unit tests for the pluggable YAML parser registry (#2709)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from faststream._internal.cli.utils.yaml_parsers import (
    _BUILTIN_PARSERS,
    _pyyaml_parser,
    get_yaml_parser,
)
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from pathlib import Path


def test_get_yaml_parser_returns_builtin_by_name() -> None:
    assert get_yaml_parser("pyyaml") is _pyyaml_parser
    assert get_yaml_parser("ruamel") is _BUILTIN_PARSERS["ruamel"]


def test_get_yaml_parser_auto_falls_back_to_pyyaml(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When `ruamel.yaml` is unavailable, `auto` resolves to PyYAML."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "ruamel.yaml" or name.startswith("ruamel"):
            err = "simulated missing ruamel"
            raise ImportError(err)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert get_yaml_parser("auto") is _pyyaml_parser
    assert get_yaml_parser(None) is _pyyaml_parser


def test_get_yaml_parser_unknown_name_raises() -> None:
    with pytest.raises(SetupError, match="Unknown YAML parser"):
        get_yaml_parser("not-a-real-parser")


def test_get_yaml_parser_dotted_path_resolves_callable(tmp_path: Path) -> None:
    """A `module:callable` reference loads a user-supplied parser."""
    plugin_dir = tmp_path / "plugin"
    plugin_dir.mkdir()
    (plugin_dir / "__init__.py").write_text("")
    (plugin_dir / "my_parser.py").write_text(
        "def parse(path):\n    return {'parsed_by': 'custom', 'path': str(path)}\n"
    )

    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        parser = get_yaml_parser("plugin.my_parser:parse")
        result = parser(plugin_dir / "__init__.py")
        assert result == {"parsed_by": "custom", "path": str(plugin_dir / "__init__.py")}
    finally:
        sys.path.remove(str(tmp_path))


def test_get_yaml_parser_dotted_path_non_callable_raises(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugin2"
    plugin_dir.mkdir()
    (plugin_dir / "__init__.py").write_text("")
    (plugin_dir / "not_callable.py").write_text("VALUE = 42\n")

    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        with pytest.raises(SetupError, match="does not resolve to a callable"):
            get_yaml_parser("plugin2.not_callable:VALUE")
    finally:
        sys.path.remove(str(tmp_path))


def test_pyyaml_parser_loads_simple_yaml(tmp_path: Path) -> None:
    f = tmp_path / "doc.yaml"
    f.write_text("a: 1\nb: hello\n")
    assert _pyyaml_parser(f) == {"a": 1, "b": "hello"}
